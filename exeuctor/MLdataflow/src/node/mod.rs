use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use timely::Data;
use timely::communication::RelayConnectAllocate;
use timely::dataflow::{Stream, Scope};
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;

use crate::input::GenericScope;
use crate::metrics::{LatencyLogger, ThroughputLogger, JCTLogger};
use crate::static_timely::timely_static_pipeline_scope::PipelineScope;

pub mod map;
pub mod filter;
pub mod inspect;
pub mod join;
pub mod aggregate;
pub mod exchange;
pub mod union;

pub use map::{MapNode, FlatMapNode, BatchedMapNode, BufferedMapNode};
pub use filter::FilterNode;
pub use inspect::InspectNode;
pub use join::{JoinNode, TimestampJoinNode, SingleItemJoinNode, TimestampSingleItemJoinNode};
pub use aggregate::{AggregateNode, TimestampAggregateNode, IncrementalAggregateNode, TimestampIncrementalAggregateNode};
pub use exchange::ExchangeNode;
pub use union::UnionNode;

/// Generic operator builder
/// The operator can takes in multiple input streams,
/// and emit an output stream
pub trait LocalOpBuilder {
    /// get the indices of required input nodes
    fn required_prev_nodes(&self) -> Vec<usize>;
    /// build the operator
    fn build(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>) -> Box<dyn GenericStream>;
    /// get throguhput metrics (avg, max, median, ...)
    fn get_throughput_logger(&self) -> Option<ThroughputLogger>;
    /// get the op's execution (compute) latency metrics (P50, P90, P99, ...)
    fn get_flow_compute_latency_logger(&self) -> Option<LatencyLogger>;
    /// get the latency from the last op emitted the output to current op finishes execution
    fn get_flow_edge_latency_logger(&self) -> Option<LatencyLogger>;
    /// get the latency from the source (when inputs to the system)
    /// all the way to current op finishes execution
    fn get_flow_path_latency_logger(&self) -> Option<LatencyLogger>;
    /// get job completion time logger
    fn get_jct_logger(&self) -> Option<JCTLogger>;
}

/// Builder for operators that emit outputs which can be sent across network,
/// i.e., output data type implements ExchangeData
pub trait ExchangeOpBuilder: LocalOpBuilder {
    fn acquire_from_input_pipeline(&self, scope: &mut dyn GenericPipelineScope, input_idx: usize) -> Box<dyn GenericStream>;
    fn build_and_register_output(&mut self, streams: &[&Box<dyn GenericStream>], config: Option<HashMap<String, Arc<dyn Any + Send + Sync>>>, scope: &mut dyn GenericPipelineScope, output_idx: usize) -> Box<dyn GenericStream>;
    fn register_pipeline_output(&self, stream: &Box<dyn GenericStream>, scope: &mut dyn GenericPipelineScope, output_idx: usize);
}

/// Trait used to store Stream<S, D> as trait objects
pub trait GenericStream {
    fn as_any(&self) -> &dyn Any;
}

impl<S: Scope + 'static, D: Data> GenericStream for Stream<S, D> {
    fn as_any(&self) -> &dyn Any { self }
}

/// Trait used to store PipelineScope<A, T> as trait objects
pub trait GenericPipelineScope: GenericScope {}

impl<A, T> GenericPipelineScope for PipelineScope<A, T>
where
    A: RelayConnectAllocate + 'static,
    T: Timestamp+Refines<()>
{
    
}