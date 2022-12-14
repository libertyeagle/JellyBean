// READ, Sep 18 2021
//! Traits and types for capturing timely dataflow streams.
//!
//! All timely dataflow streams can be captured, but there are many ways to capture
//! these streams. A stream may be `capture_into`'d any type implementing `EventPusher`,
//! and there are several default implementations, including a linked-list, Rust's MPSC
//! queue, and a binary serializer wrapping any `W: Write`.

use crate::Data;
use crate::dataflow::{Scope, Stream};
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::channels::pullers::Counter as PullCounter;
// use the OperatorBuilder from operator_raw
use crate::dataflow::operators::generic::builder_raw::OperatorBuilder;

use crate::progress::ChangeBatch;
use crate::progress::Timestamp;

use super::{Event, EventPusher};

/// Capture a stream of timestamped data for later replay.
pub trait Capture<T: Timestamp, D: Data> {
    /// Captures a stream of timestamped data for later replay.
    ///
    /// # Examples
    ///
    /// The type `Rc<EventLink<T,D>>` implements a typed linked list,
    /// and can be captured into and replayed from.
    ///
    /// ```rust
    /// use std::rc::Rc;
    /// use std::sync::{Arc, Mutex};
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Capture, ToStream, Inspect};
    /// use timely::dataflow::operators::capture::{EventLink, Replay, Extract};
    ///
    /// // get send and recv endpoints, wrap send to share
    /// let (send, recv) = ::std::sync::mpsc::channel();
    /// let send = Arc::new(Mutex::new(send));
    ///
    /// timely::execute(timely::Config::thread(), move |worker| {
    ///
    ///     // this is only to validate the output.
    ///     let send = send.lock().unwrap().clone();
    ///
    ///     // these are to capture/replay the stream.
    ///     let handle1 = Rc::new(EventLink::new());
    ///     let handle2 = Some(handle1.clone());
    ///
    ///     worker.dataflow::<u64,_,_>(|scope1|
    ///         (0..10).to_stream(scope1)
    ///                .capture_into(handle1)
    ///     );
    ///
    ///     worker.dataflow(|scope2| {
    ///         handle2.replay_into(scope2)
    ///                .capture_into(send)
    ///     });
    /// }).unwrap();
    ///
    /// assert_eq!(recv.extract()[0].1, (0..10).collect::<Vec<_>>());
    /// ```
    ///
    /// The types `EventWriter<T, D, W>` and `EventReader<T, D, R>` can be
    /// captured into and replayed from, respectively. They use binary writers
    /// and readers respectively, and can be backed by files, network sockets,
    /// etc.
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::net::{TcpListener, TcpStream};
    /// use std::sync::{Arc, Mutex};
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Capture, ToStream, Inspect};
    /// use timely::dataflow::operators::capture::{EventReader, EventWriter, Replay, Extract};
    ///
    /// // get send and recv endpoints, wrap send to share
    /// let (send0, recv0) = ::std::sync::mpsc::channel();
    /// let send0 = Arc::new(Mutex::new(send0));
    ///
    /// timely::execute(timely::Config::thread(), move |worker| {
    ///
    ///     // this is only to validate the output.
    ///     let send0 = send0.lock().unwrap().clone();
    ///
    ///     // these allow us to capture / replay a timely stream.
    ///     let list = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///     let send = TcpStream::connect("127.0.0.1:8000").unwrap();
    ///     let recv = list.incoming().next().unwrap().unwrap();
    ///
    ///     recv.set_nonblocking(true).unwrap();
    ///
    ///     worker.dataflow::<u64,_,_>(|scope1|
    ///         (0..10u64)
    ///             .to_stream(scope1)
    ///             .capture_into(EventWriter::new(send))
    ///     );
    ///
    ///     worker.dataflow::<u64,_,_>(|scope2| {
    ///         Some(EventReader::<_,u64,_>::new(recv))
    ///             .replay_into(scope2)
    ///             .capture_into(send0)
    ///     });
    /// }).unwrap();
    ///
    /// assert_eq!(recv0.extract()[0].1, (0..10).collect::<Vec<_>>());
    /// ```
    fn capture_into<P: EventPusher<T, D>+'static>(&self, pusher: P);

    /// Captures a stream using Rust's MPSC channels.
    fn capture(&self) -> ::std::sync::mpsc::Receiver<Event<T, D>> {
        let (send, recv) = ::std::sync::mpsc::channel();
        self.capture_into(send);
        recv
    }
}

impl<S: Scope, D: Data> Capture<S::Timestamp, D> for Stream<S, D> {
    fn capture_into<P: EventPusher<S::Timestamp, D>+'static>(&self, mut event_pusher: P) {

        // note that we use the builder_raw's OperatorBuilder
        // builder_rc's OperatorBuilder give us some initial capabilities
        // it automatically give us access to frontier by pulling the frontier updates
        // it also automatically pushes any #consumed messages, #procuded messages, capabilities changes that we made
        // to SharedProgress

        // but in our capture_into case, we just care about the frontier change,
        // this_operator's progress is not important
        // it will not have capabilities
        // it does not produce messages
        // we can just have direct (raw) access to SharedProgress
        let mut builder = OperatorBuilder::new("Capture".to_owned(), self.scope());
        // new_input() returns a pusher to the input port
        // Pull<Bundle<T, D>>
        let mut input = PullCounter::new(builder.new_input(self, Pipeline));
        let mut started = false;

        builder.build(
            move |progress| {

                if !started {
                    // discard initial capability.
                    // frontiers are based on the locations, instead of the number of messages / capabilities
                    // we do not care about the frontier caused by the initial capabilities
                    // we just care about frontier change after the dataflow runs
                    progress.frontiers[0].update(S::Timestamp::minimum(), -1);
                    started = true;
                }
                if !progress.frontiers[0].is_empty() {
                    // transmit any frontier progress.
                    // drain the frontier change from SharedProgress
                    // since we just care about the updates
                    // we don't want to repeatedly report the same frontier
                    let to_send = ::std::mem::replace(&mut progress.frontiers[0], ChangeBatch::new());
                     event_pusher.push(Event::Progress(to_send.into_inner()));
                }

                use crate::communication::message::RefOrMut;

                // turn each received message into an event.
                // message pulled from the puller returned from the ParallelizationContract when we create the input
                while let Some(message) = input.next() {
                    let (time, data) = match message.as_ref_or_mut() {
                        // reference's type is dataflow/channels' Message
                        RefOrMut::Ref(reference) => (&reference.time, RefOrMut::Ref(&reference.data)),
                        RefOrMut::Mut(reference) => (&reference.time, RefOrMut::Mut(&mut reference.data)),
                    };
                    let vector = data.replace(Vec::new());
                    event_pusher.push(Event::Messages(time.clone(), vector));
                }
                // but we still need to tell the dataflow progress tracking module
                // the number of messages we consumed
                input.consumed().borrow_mut().drain_into(&mut progress.consumeds[0]);
                false
            }
        );
    }
}
