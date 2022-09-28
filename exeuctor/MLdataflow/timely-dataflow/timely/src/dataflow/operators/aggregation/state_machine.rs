// READ, Sep 14 2021
//! General purpose state transition operator.
use std::hash::Hash;
use std::collections::HashMap;

use crate::{Data, ExchangeData};
use crate::dataflow::{Stream, Scope};
use crate::dataflow::operators::generic::operator::Operator;
use crate::dataflow::channels::pact::Exchange;

/// Generic state-transition machinery: each key has a state, and receives a sequence of events.
/// Events are applied in time-order, but no other promises are made. Each state transition can
/// produce output, which is sent.
///
/// `state_machine` will buffer inputs if earlier inputs may still arrive. it will directly apply
/// updates for the current time reflected in the notificator, though. In the case of partially
/// ordered times, the only guarantee is that updates are not applied out of order, not that there
/// is some total order on times respecting the total order (updates may be interleaved).

/// Provides the `state_machine` method.
pub trait StateMachine<S: Scope, K: ExchangeData+Hash+Eq, V: ExchangeData> {
    /// Tracks a state for each presented key, using user-supplied state transition logic.
    ///
    /// The transition logic `fold` may mutate the state, and produce both output records and
    /// a `bool` indicating that it is appropriate to deregister the state, cleaning up once
    /// the state is no longer helpful.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    /// use timely::dataflow::operators::aggregation::StateMachine;
    ///
    /// timely::example(|scope| {
    ///
    ///     // these results happen to be right, but aren't guaranteed.
    ///     // the system is at liberty to re-order within a timestamp.
    ///     let result = vec![(0,0), (0,2), (0,6), (0,12), (0,20),
    ///                       (1,1), (1,4), (1,9), (1,16), (1,25)];
    ///
    ///         (0..10).to_stream(scope)
    ///                .map(|x| (x % 2, x))
    ///                .state_machine(
    ///                    |_key, val, agg| { *agg += val; (false, Some((*_key, *agg))) },
    ///                    |key| *key as u64
    ///                )
    ///                .inspect(move |x| assert!(result.contains(x)));
    /// });
    /// ```
    fn state_machine<
        // state machine returns type R (e.g., (key, agg))
        R: Data,                                    // output type
        D: Default+'static,                         // per-key state (data)
        I: IntoIterator<Item=R>,                    // type of output iterator
        F: Fn(&K, V, &mut D)->(bool, I)+'static,    // state update logic
        H: Fn(&K)->u64+'static,                     // "hash" function for keys
    >(&self, fold: F, hash: H) -> Stream<S, R> where S::Timestamp : Hash+Eq ;
    // S: type of scope
    // R: type of stream data
}

// an input_stream of data type (K, V) can call
// K must implements Hash Eq
// both K and V should be able to exchange between workers via timely_communication
impl<S: Scope, K: ExchangeData+Hash+Eq, V: ExchangeData> StateMachine<S, K, V> for Stream<S, (K, V)> {
    fn state_machine<
            R: Data,                                    // output type
            D: Default+'static,                         // per-key state (data)
            I: IntoIterator<Item=R>,                    // type of output iterator
            // F takes the key (by reference), value (take ownership) and
            // returns an iterteable over output
            // as well as a bool value to indicate whether to remove this key from the state machine
            F: Fn(&K, V, &mut D)->(bool, I)+'static,    // state update logic
            // key hashing function
            H: Fn(&K)->u64+'static,                     // "hash" function for keys
        >(&self, fold: F, hash: H) -> Stream<S, R> where S::Timestamp : Hash+Eq {

        // hash map maps from timestamp to Vec<(K,V)>
        let mut pending: HashMap<_, Vec<(K, V)>> = HashMap::new();   // times -> (keys -> state)
        let mut states = HashMap::new();    // keys -> state

        let mut vector = Vec::new();
        // hash function in exchange should be FnMut(&D)->u64
        // where D is the data type of the stream
        // in this case, &D is &(K, V)
        self.unary_notify(Exchange::new(move |&(ref k, _)| hash(k)), "StateMachine", vec![], move |input, output, notificator| {

            // go through each time with data, process each (key, val) pair.
            // notificator.for_each takes a closure of:
            // FnMut(Capability<T>, u64, &mut Notificator<T>)
            notificator.for_each(|time,_,_| {
                // all inputs for timestamp = time have been delivered at this point
                // if there is data with timestamp = time
                if let Some(pend) = pending.remove(time.time()) {
                    let mut session = output.session(&time);
                    for (key, val) in pend {
                        let (remove, output) = {
                            let state = states.entry(key.clone()).or_insert_with(Default::default);
                            // aggregate
                            fold(&key, val, state)
                        };
                        if remove { states.remove(&key); }
                        // output an iterator for each consumed key
                        session.give_iterator(output.into_iter());
                    }
                }
            });

            // stash each input and request a notification when ready
            input.for_each(|time, data| {

                // use vector to take data
                data.swap(&mut vector);

                // stash if not time yet
                // since the state depends on the order of fold call
                // if the arriving KV pairs' time is not less than that in the frontier
                // we must stash the KV pair for fold to process it later
                // to ensure timely order
                if notificator.frontier(0).less_than(time.time()) {
                    pending.entry(time.time().clone()).or_insert_with(Vec::new).extend(vector.drain(..));
                    notificator.notify_at(time.retain());
                }
                else {
                    // else we can process immediately
                    // each input message arrvies with a CapabilityRef
                    // we can create a new capability for the output if we want
                    // or we can just use this CapabilityRef to create a new capability
                    // for the operator (output port) to retain
                    let mut session = output.session(&time);
                    for (key, val) in vector.drain(..) {
                        let (remove, output) = {
                            let state = states.entry(key.clone()).or_insert_with(Default::default);
                            fold(&key, val, state)
                        };
                        if remove { states.remove(&key); }
                        session.give_iterator(output.into_iter());
                    }
                }
            });
        })
    }
}
