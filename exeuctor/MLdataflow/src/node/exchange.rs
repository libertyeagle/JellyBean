use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use timely::communication::RelayConnectAllocate;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;
use timely::dataflow::{Scope, Stream};
use timely::ExchangeData;
use timely::dataflow::operators::Exchange;

use crate::TimestampData;
use crate::static_timely::timely_static_pipeline_scope::PipelineScope;
use crate::operators_timely::Map;

use super::{LocalOpBuilder, ExchangeOpBuilder};
use super::GenericStream;
use super::GenericPipelineScope;

/// Exchange
/// Exchange records between workers within a pipeline
pub struct ExchangeNode<D, L, S> 
where
    L: Fn(&D) -> u64 + 'static,
    S: Scope + 'static 
{
    prev_index: usize,
    logic: Option<L>,
    phantom: PhantomData<D>,
    phantom_scope: PhantomData<S>
}

impl<D, L, S> ExchangeNode<D, L, S>
where
    L: Fn(&D) -> u64 + 'static,
    S: Scope + 'static 
{
    pub fn new(prev_index: usize, logic: L) -> Self {
        ExchangeNode {
            prev_index,
            logic: Some(logic),
            phantom: PhantomData,
            phantom_scope: PhantomData
        }
    }
}

impl<D: ExchangeData, L, S> LocalOpBuilder for ExchangeNode<D, L, S>
where
    L: Fn(&D) -> u64 + 'static,
    S: Scope + 'static
{
    fn required_prev_nodes(&self) -> Vec<usize> {
        vec![self.prev_index]
    }

    fn build(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>) -> Box<dyn GenericStream> {
        let sim_network_latency = if let Some(config) = &config {
            config.get("simulate_network_latency").and_then(|val| val.downcast_ref::<i64>()).map(|val| *val)
        }
        else { None };

        let stream_in = streams[0];
        let stream_in = stream_in.as_any().downcast_ref::<Stream<S, TimestampData<D>>>().unwrap();
        let stream_in = stream_in.map_in_place(move |x, mut net_lat| {
            if let Some(sim_net_lat) = sim_network_latency {
                net_lat = sim_net_lat;
            }
            if net_lat < 0 { net_lat = 0 }    
            x.total_exec_net_latency += net_lat
        });
        let logic = self.logic.take().unwrap();
        let stream_out = stream_in.exchange(move |x| (logic)(&x.data));
        Box::new(stream_out)
    }

    fn get_throughput_logger(&self) -> Option<crate::metrics::ThroughputLogger> {
        None
    }

    fn get_flow_compute_latency_logger(&self) -> Option<crate::metrics::LatencyLogger> {
        None
    }

    fn get_flow_edge_latency_logger(&self) -> Option<crate::metrics::LatencyLogger> {
        None
    }

    fn get_flow_path_latency_logger(&self) -> Option<crate::metrics::LatencyLogger> {
        None
    }

    fn get_jct_logger(&self) -> Option<crate::metrics::JCTLogger> {
        None
    }
}

impl<D: ExchangeData, L, A, T> ExchangeOpBuilder for ExchangeNode<D, L, PipelineScope<A, T>>
where
    L: Fn(&D) -> u64 + 'static,
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>
{
    fn acquire_from_input_pipeline(&self, scope: &mut dyn GenericPipelineScope, input_idx: usize) -> Box<dyn GenericStream> {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = scope.acquire_pipeline_input::<TimestampData<D>>(input_idx);
        Box::new(stream)
    }

    fn build_and_register_output(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>, scope: &mut dyn GenericPipelineScope, output_idx: usize) -> Box<dyn GenericStream> {
        let stream = (self as &mut dyn LocalOpBuilder).build(streams, config);
        self.register_pipeline_output(&stream, scope, output_idx);
        stream
    }

    fn register_pipeline_output(&self, stream: &Box<dyn GenericStream>, scope: &mut dyn GenericPipelineScope, output_idx: usize) {
        let scope = scope.as_any_mut().downcast_mut::<PipelineScope<A, T>>().unwrap();
        let stream = stream.as_any().downcast_ref::<Stream<PipelineScope<A, T>, TimestampData<D>>>().unwrap();
        scope.register_pipeline_output_balanced_exchange(stream, output_idx);
    }
}