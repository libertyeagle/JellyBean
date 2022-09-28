//! Extension trait and implementation for observing and action on streamed data.
use timely::Data;
use timely::communication::MessageLatency;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::operators::generic::Operator;

/// Methods to inspect records and batches of records on a stream.
pub trait Inspect<G: Scope, D: Data> {
    fn inspect(&self, mut func: impl FnMut(&D, MessageLatency) -> MessageLatency +'static) -> Stream<G, (D, MessageLatency)> {
        self.inspect_batch(move |_, data, net_lat, all_exec_latency| {
            for datum in data.iter() { 
                let exec_lat = func(datum, net_lat); 
                all_exec_latency.push(exec_lat);
            }
        })
    }

    fn inspect_time(&self, mut func: impl FnMut(&G::Timestamp, &D, MessageLatency) -> MessageLatency +'static) -> Stream<G, (D, MessageLatency)> {
        self.inspect_batch(move |time, data, net_lat, all_exec_latency| {
            all_exec_latency.clear();
            for datum in data.iter() {
                let exec_lat = func(&time, &datum, net_lat);
                all_exec_latency.push(exec_lat);
            }
        })
    }

    fn inspect_batch<'b>(&self, func: impl for<'a> FnMut(&'a G::Timestamp, &'a [D], MessageLatency, &'a mut Vec<MessageLatency>) +'static) -> Stream<G, (D, MessageLatency)>;
}

impl<G: Scope, D: Data> Inspect<G, D> for Stream<G, D> {

    fn inspect_batch(&self, mut func: impl for<'a> FnMut(&'a G::Timestamp, &'a [D], MessageLatency, &'a mut Vec<MessageLatency>) +'static) -> Stream<G, (D, MessageLatency)> {
        let mut vector = Vec::new();
        let mut all_exec_latency = Vec::new();
        let stream_out = self.unary(Pipeline, "InspectBatch", move |_,_| move |input, output| {
            input.for_each_with_latency(|time, data, lat| {
                let net_lat = if let Some(lat) = lat { lat }
                else { 0 };
                data.swap(&mut vector);
                func(&time, &vector[..], net_lat, &mut all_exec_latency);
                let output_iter = vector.drain(..).zip(all_exec_latency.drain(..)).map(|(x, exec_lat)| (x, exec_lat + net_lat));
                output.session(&time).give_iterator(output_iter);
            });
        });
        stream_out
    }
}
