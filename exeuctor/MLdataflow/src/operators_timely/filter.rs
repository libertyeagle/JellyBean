use timely::Data;
use timely::communication::MessageLatency;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::operators::generic::operator::Operator;

/// Extension trait for filtering.
pub trait Filter<G: Scope, D: Data> {
    fn filter<P: FnMut(&D, MessageLatency) -> (bool, MessageLatency) +'static>(&self, predicate: P) -> Stream<G, (D, MessageLatency)>;
}

impl<G: Scope, D: Data> Filter<G, D> for Stream<G, D> {
    fn filter<P: FnMut(&D, MessageLatency) -> (bool, MessageLatency) +'static>(&self, mut predicate: P) -> Stream<G, (D, MessageLatency)> {
        let mut vector = Vec::new();
        let mut all_exec_lat = Vec::new();
        let stream_out = self.unary(Pipeline, "Filter", move |_,_| move |input, output| {
            input.for_each_with_latency(|time, data, lat| {
                let net_lat = if let Some(lat) = lat { lat }
                else { 0 };
                data.swap(&mut vector);
                vector.retain(|x| {
                    let (pred, exec_lat) =  predicate(x, net_lat);
                    if pred {
                        all_exec_lat.push(exec_lat + net_lat);
                    }
                    pred
                });
                let output_iter = vector.drain(..).zip(all_exec_lat.drain(..));
                if output_iter.len() > 0 {
                    output.session(&time).give_iterator(output_iter);
                }
            });
        });
        stream_out
    }
}
