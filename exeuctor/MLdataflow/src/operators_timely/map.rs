// READ, Sep 18 2021
//! Extension methods for `Stream` based on record-by-record transformation.

use std::collections::HashMap;

use timely::Data;
use timely::communication::MessageLatency;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;

/// Extension trait for `Stream`.
pub trait Map<S: Scope, D: Data> {
    fn map<D2: Data, L: FnMut(D, MessageLatency)->D2+'static>(&self, logic: L) -> Stream<S, D2>;
    fn map_in_place<L: FnMut(&mut D, MessageLatency)+'static>(&self, logic: L) -> Stream<S, D>;
    fn flat_map<I: IntoIterator, L: FnMut(D, MessageLatency)->I+'static>(&self, logic: L) -> Stream<S, I::Item> where I::Item: Data;
    fn batched_map<D2: Data, I2: IntoIterator<Item=D2>, L: FnMut(Vec<(D, MessageLatency)>)->I2+'static>(&self, batch_size: usize, logic: L) -> Stream<S, D2>;
    fn buffered_map<D2: Data, L: FnMut(D, MessageLatency)->D2+'static>(&self, buffer_size: usize, logic: L) -> Stream<S, D2>;
}

impl<S: Scope, D: Data> Map<S, D> for Stream<S, D> {
    fn map<D2: Data, L: FnMut(D, MessageLatency)->D2+'static>(&self, mut logic: L) -> Stream<S, D2> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "Map", move |_,_| move |input, output| {
            input.for_each_with_latency(|time, data, lat| {
                let lat = if let Some(lat) = lat { lat }
                else { 0 };
                data.swap(&mut vector);
                // convert the input vector (of messages) to another vector of type D2
                output.session(&time).give_iterator(vector.drain(..).map(|x| logic(x, lat)));
            });
        })
    }
    
    fn map_in_place<L: FnMut(&mut D, MessageLatency)+'static>(&self, mut logic: L) -> Stream<S, D> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "MapInPlace", move |_,_| move |input, output,| {
            input.for_each_with_latency(|time, data, lat| {
                let lat = if let Some(lat) = lat { lat }
                else { 0 };
                data.swap(&mut vector);
                // inplace mapping
                for datum in vector.iter_mut() { logic(datum, lat); }
                output.session(&time).give_vec(&mut vector);
            })
        })
    }
  
    fn flat_map<I: IntoIterator, L: FnMut(D, MessageLatency)->I+'static>(&self, mut logic: L) -> Stream<S, I::Item> where I::Item: Data {
        let mut vector = Vec::new();
        // each input record may produce multiple output records
        self.unary(Pipeline, "FlatMap", move |_,_| move |input, output| {
            input.for_each_with_latency(|time, data, lat| {
                let lat = if let Some(lat) = lat { lat }
                else { 0 };
                data.swap(&mut vector);
                output.session(&time).give_iterator(vector.drain(..).flat_map(|x| logic(x, lat).into_iter()));
            });
        })
    }

    fn batched_map<D2: Data, I2: IntoIterator<Item=D2>, L: FnMut(Vec<(D, MessageLatency)>)->I2+'static>(&self, batch_size: usize, mut logic: L) -> Stream<S, D2> {
        let mut vector = Vec::new();
        let mut input_buffer = HashMap::new();
        let stream_out = self.unary_notify(Pipeline, "BatchMap", None, move |input, output, notificator| {
            input.for_each_with_latency(|time, data, lat| {
                let lat = if let Some(lat) = lat { lat }
                else { 0 };
                data.swap(&mut vector);
                // convert the input vector (of messages) to another vector of type D2
                let timestamp_buffer = input_buffer.entry(time.time().clone()).or_insert(Vec::with_capacity(batch_size));
                for x in vector.drain(..) {
                    timestamp_buffer.push((x, lat));
                    if timestamp_buffer.len() >= batch_size {
                        let batched_output = logic(timestamp_buffer.drain(..).collect());
                        output.session(&time).give_iterator(batched_output.into_iter());
                    }
                }
                notificator.notify_at(time.retain());
            });
            notificator.for_each(|time, _, _| {
                if let Some(timestamp_buffer) = input_buffer.remove(time.time()) {
                    if !timestamp_buffer.is_empty() {
                        let batched_output = logic(timestamp_buffer);
                        output.session(&time).give_iterator(batched_output.into_iter());
                    }
                }
            });
        });
        stream_out     
    }

    fn buffered_map<D2: Data, L: FnMut(D, MessageLatency)->D2+'static>(&self, buffer_size: usize, mut logic: L) -> Stream<S, D2> {
        let mut vector = Vec::new();
        let mut buffer = HashMap::new();
        let stream_out = self.unary_notify(Pipeline, "BufferMap", None, move |input, output, notificator| {
            input.for_each_with_latency(|time, data, lat| {
                let lat = if let Some(lat) = lat { lat }
                else { 0 };
                data.swap(&mut vector);
                // convert the input vector (of messages) to another vector of type D2
                let timestamp_buffer = buffer.entry(time.time().clone()).or_insert(Vec::with_capacity(buffer_size));
                for x in vector.drain(..) {
                    let mapped = logic(x, lat);
                    timestamp_buffer.push(mapped);
                    if timestamp_buffer.len() >= buffer_size {
                        output.session(&time).give_iterator(timestamp_buffer.drain(..))
                    }
                }
                notificator.notify_at(time.retain());
            });
            notificator.for_each(|time, _, _| {
                if let Some(timestamp_buffer) = buffer.remove(time.time()) {
                    if !timestamp_buffer.is_empty() {
                        output.session(&time).give_iterator(timestamp_buffer.into_iter());
                    }
                }
            });
        });      
        stream_out     
    }
}